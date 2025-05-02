'use client';

import { useState, useEffect } from 'react';
import { Post } from '@/lib/types';
import { getUserPosts, createPost } from '@/lib/api';
import { PostCard } from '@/components/post-card';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';

interface UserPostsProps {
  userId: string;
}

export function UserPosts({ userId }: UserPostsProps) {
  const [posts, setPosts] = useState<Post[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [newPostContent, setNewPostContent] = useState('');
  const [creatingPost, setCreatingPost] = useState(false);

  const fetchPosts = async () => {
    if (!userId) return;

    try {
      setLoading(true);
      const postsData = await getUserPosts(userId);
      setPosts(postsData);
    } catch (err) {
      setError('Failed to load posts');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPosts();
  }, [userId]);

  const handleCreatePost = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!newPostContent.trim()) return;
    
    try {
      setCreatingPost(true);
      await createPost(userId, newPostContent);
      setNewPostContent('');
      // Refresh posts
      fetchPosts();
    } catch (err) {
      console.error('Failed to create post:', err);
      setError('Failed to create post');
    } finally {
      setCreatingPost(false);
    }
  };

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Your Posts</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {[1, 2].map((i) => (
              <div key={i} className="rounded-lg border p-4">
                <div className="flex items-center space-x-3">
                  <Skeleton className="h-10 w-10 rounded-full" />
                  <div className="space-y-2">
                    <Skeleton className="h-4 w-[200px]" />
                    <Skeleton className="h-3 w-[150px]" />
                  </div>
                </div>
                <div className="mt-4 space-y-2">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-3/4" />
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Your Posts</CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleCreatePost} className="mb-6">
          <div className="space-y-4">
            <Input
              placeholder="What's on your mind?"
              value={newPostContent}
              onChange={(e) => setNewPostContent(e.target.value)}
              disabled={creatingPost}
            />
            <Button 
              type="submit" 
              disabled={creatingPost || !newPostContent.trim()}
              className="w-full"
            >
              {creatingPost ? 'Posting...' : 'Post'}
            </Button>
          </div>
        </form>

        {error && (
          <div className="mb-4 rounded bg-red-50 p-2 text-red-500">
            {error}
          </div>
        )}

        {posts.length === 0 ? (
          <div className="py-8 text-center text-gray-500">
            <p>No posts yet</p>
            <p className="text-sm">Share your thoughts above!</p>
          </div>
        ) : (
          <div className="space-y-4">
            {posts.map((post) => (
              <PostCard 
                key={post.post_id} 
                post={post} 
                currentUserId={userId}
                onRefresh={fetchPosts}
              />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
} 