'use client';

import { useState, useCallback } from 'react';
import { UserSelect } from '@/components/user-select';
import { UserProfile } from '@/components/user-profile';
import { UserFeed } from '@/components/user-feed';
import { UserFollowers } from '@/components/user-followers';
import { UserPosts } from '@/components/user-posts';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

export default function Home() {
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);

  // Use useCallback to prevent unnecessary re-renders
  const handleUserChange = useCallback((userId: string) => {
    console.log("User changed to:", userId); // Debug log
    setSelectedUserId(userId);
  }, []);

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="border-b bg-white py-4 shadow-sm">
        <div className="container mx-auto px-4">
          <h1 className="text-2xl font-bold text-blue-600">Social News Feed</h1>
        </div>
      </header>

      <main className="container mx-auto px-4 py-8">
        <div className="mb-8">
          <h2 className="mb-4 text-xl font-semibold">Select User</h2>
          <UserSelect onUserChange={handleUserChange} />
        </div>

        {selectedUserId && (
          <div className="grid gap-8 md:grid-cols-3">
            <div className="md:col-span-1 space-y-8">
              <UserProfile userId={selectedUserId} />
              <UserFollowers userId={selectedUserId} />
            </div>
            
            <div className="md:col-span-2">
              <Tabs defaultValue="feed">
                <TabsList className="w-full">
                  <TabsTrigger value="feed" className="flex-1">Feed</TabsTrigger>
                  <TabsTrigger value="posts" className="flex-1">Posts</TabsTrigger>
                </TabsList>
                <TabsContent value="feed" className="mt-6">
                  <UserFeed userId={selectedUserId} />
                </TabsContent>
                <TabsContent value="posts" className="mt-6">
                  <UserPosts userId={selectedUserId} />
                </TabsContent>
              </Tabs>
            </div>
          </div>
        )}
      </main>

      <footer className="border-t bg-white py-6 text-center text-gray-500">
        <div className="container mx-auto px-4">
          <p>© {new Date().getFullYear()} Social News Feed UI</p>
        </div>
      </footer>
    </div>
  );
}
