'use client';

import { useState, useEffect } from 'react';
import { User } from '@/lib/types';
import { getFollowers, getFollowing, followUser, unfollowUser } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';

interface UserFollowersProps {
  userId: string;
}

export function UserFollowers({ userId }: UserFollowersProps) {
  const [followers, setFollowers] = useState<User[]>([]);
  const [following, setFollowing] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState('followers');
  const [processingFollowIds, setProcessingFollowIds] = useState<Set<string>>(new Set());

  const fetchData = async () => {
    if (!userId) return;

    try {
      setLoading(true);
      
      const [followersData, followingData] = await Promise.all([
        getFollowers(userId),
        getFollowing(userId)
      ]);
      
      setFollowers(followersData);
      setFollowing(followingData);
    } catch (err) {
      setError('Failed to load followers data');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [userId]);

  // Check if a user is being followed by the current user
  const isFollowing = (otherUserId: string) => {
    return following.some(user => user.user_id === otherUserId);
  };

  // Handle follow/unfollow button click
  const handleFollowToggle = async (otherUserId: string, shouldFollow: boolean) => {
    if (processingFollowIds.has(otherUserId)) return; // Prevent duplicate requests
    
    try {
      setProcessingFollowIds(prev => new Set(prev).add(otherUserId));
      
      // Optimistic UI update - immediately update the UI
      if (shouldFollow) {
        // Optimistically add to following list
        setFollowing(prev => {
          const otherUser = followers.find(u => u.user_id === otherUserId);
          if (otherUser && !prev.some(u => u.user_id === otherUserId)) {
            return [...prev, otherUser];
          }
          return prev;
        });
      } else {
        // Optimistically remove from following list
        setFollowing(prev => prev.filter(user => user.user_id !== otherUserId));
      }
      
      // Make the actual API call
      if (shouldFollow) {
        await followUser(userId, otherUserId);
      } else {
        await unfollowUser(userId, otherUserId);
      }
      
      // Refresh data from server to ensure accuracy
      await fetchData();
    } catch (error) {
      console.error('Failed to update follow status:', error);
      // Revert optimistic update if there was an error
      await fetchData();
    } finally {
      setProcessingFollowIds(prev => {
        const newSet = new Set(prev);
        newSet.delete(otherUserId);
        return newSet;
      });
    }
  };

  const renderUsers = (users: User[]) => {
    if (users.length === 0) {
      return (
        <div className="py-6 text-center text-gray-500">
          No users to display
        </div>
      );
    }

    return (
      <div className="divide-y">
        {users.map((user) => {
          const isCurrentlyFollowing = isFollowing(user.user_id);
          const isProcessing = processingFollowIds.has(user.user_id);
          
          return (
            <div key={user.user_id} className="flex items-center justify-between py-3">
              <div className="flex items-center space-x-3">
                <Avatar>
                  <AvatarImage src={user.profile_picture_url} alt={user.username} />
                  <AvatarFallback>{user.username.substring(0, 2).toUpperCase()}</AvatarFallback>
                </Avatar>
                <div>
                  <p className="font-medium">{user.username}</p>
                  <p className="text-xs text-gray-500">{user.email}</p>
                </div>
              </div>
              {activeTab === 'followers' ? (
                // For followers tab
                <Button 
                  variant={isCurrentlyFollowing ? "outline" : "default"} 
                  size="sm"
                  disabled={isProcessing}
                  onClick={() => handleFollowToggle(user.user_id, !isCurrentlyFollowing)}
                >
                  {isProcessing ? "Processing..." : 
                   isCurrentlyFollowing ? "Following" : "Follow Back"}
                </Button>
              ) : (
                // For following tab
                <Button 
                  variant="outline" 
                  size="sm"
                  disabled={isProcessing}
                  onClick={() => handleFollowToggle(user.user_id, false)}
                >
                  {isProcessing ? "Processing..." : "Unfollow"}
                </Button>
              )}
            </div>
          );
        })}
      </div>
    );
  };

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Network</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="divide-y">
            {[1, 2, 3].map((i) => (
              <div key={i} className="flex items-center justify-between py-3">
                <div className="flex items-center space-x-3">
                  <Skeleton className="h-10 w-10 rounded-full" />
                  <div>
                    <Skeleton className="h-4 w-[120px]" />
                    <Skeleton className="mt-1 h-3 w-[200px]" />
                  </div>
                </div>
                <Skeleton className="h-8 w-20" />
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Network</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="rounded bg-red-50 p-4 text-red-500">
            Error: {error}
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Network</CardTitle>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="followers" onValueChange={setActiveTab}>
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="followers">
              Followers ({followers.length})
            </TabsTrigger>
            <TabsTrigger value="following">
              Following ({following.length})
            </TabsTrigger>
          </TabsList>
          <TabsContent value="followers" className="mt-4">
            {renderUsers(followers)}
          </TabsContent>
          <TabsContent value="following" className="mt-4">
            {renderUsers(following)}
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
} 